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

use bytes::{BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::contract::{Key, StoreRevision, validate_page_size};
use super::error::{StateStoreError, StateStoreErrorKind};
use super::limits::StateStoreLimits;

const CODEC_VERSION: u8 = 1;
const FORWARD_CODE: u8 = 0;
const REVERSE_CODE: u8 = 1;
const REQUEST_FINGERPRINT_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
    Forward,
    Reverse,
}

impl Direction {
    const fn code(self) -> u8 {
        match self {
            Self::Forward => FORWARD_CODE,
            Self::Reverse => REVERSE_CODE,
        }
    }

    fn from_code(code: u8) -> Result<Self, StateStoreError> {
        match code {
            FORWARD_CODE => Ok(Self::Forward),
            REVERSE_CODE => Ok(Self::Reverse),
            _ => Err(invalid_token()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRange {
    pub start: Key,
    pub end: Key,
}

impl KeyRange {
    pub fn new(start: Key, end: Key) -> Result<Self, StateStoreError> {
        if start >= end {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "range start must be less than range end",
            ));
        }
        Ok(Self { start, end })
    }

    pub fn for_prefix(prefix: Key) -> Result<Self, StateStoreError> {
        let mut successor = prefix.as_bytes().to_vec();
        let position = successor
            .iter()
            .rposition(|byte| *byte != u8::MAX)
            .ok_or_else(|| {
                StateStoreError::new(
                    StateStoreErrorKind::InvalidRequest,
                    "prefix has no finite successor",
                )
            })?;
        let byte = successor.get_mut(position).ok_or_else(invalid_token)?;
        *byte += 1;
        successor.truncate(position + 1);
        Self::new(prefix, Key::try_from(Bytes::from(successor))?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeRequest {
    pub range: KeyRange,
    pub direction: Direction,
    pub page_size: usize,
    pub continuation: Option<ContinuationToken>,
}

impl RangeRequest {
    pub fn validate(&self, limits: &StateStoreLimits) -> Result<(), StateStoreError> {
        validate_page_size(self.page_size, limits.max_page_size)?;
        if let Some(continuation) = &self.continuation {
            continuation.resume_after(self)?;
        }
        Ok(())
    }

    pub fn continuation_after(&self, last_key: &Key) -> Result<ContinuationToken, StateStoreError> {
        if last_key < &self.range.start || last_key >= &self.range.end {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "continuation key is outside the requested range",
            ));
        }
        let fingerprint = request_fingerprint(&self.range, self.direction)?;
        let key_len = u32::try_from(last_key.as_bytes().len()).map_err(|_| invalid_token())?;
        let capacity = 2usize
            .checked_add(REQUEST_FINGERPRINT_BYTES)
            .and_then(|size| size.checked_add(4))
            .and_then(|size| size.checked_add(last_key.as_bytes().len()))
            .ok_or_else(invalid_token)?;
        let mut encoded = BytesMut::with_capacity(capacity);
        encoded.put_u8(CODEC_VERSION);
        encoded.put_u8(self.direction.code());
        encoded.extend_from_slice(&fingerprint);
        encoded.put_u32(key_len);
        encoded.extend_from_slice(last_key.as_bytes());
        Ok(ContinuationToken(encoded.freeze()))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContinuationToken(Bytes);

impl ContinuationToken {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    pub fn resume_after(&self, request: &RangeRequest) -> Result<Key, StateStoreError> {
        let mut reader = CheckedReader::new(self.as_bytes(), invalid_token);
        if reader.read_u8()? != CODEC_VERSION {
            return Err(invalid_token());
        }
        let direction = Direction::from_code(reader.read_u8()?)?;
        if direction != request.direction {
            return Err(invalid_token());
        }
        let fingerprint = reader.read_exact(REQUEST_FINGERPRINT_BYTES)?;
        let expected = request_fingerprint(&request.range, request.direction)?;
        if fingerprint != expected.as_slice() {
            return Err(invalid_token());
        }
        let key_len = usize::try_from(reader.read_u32()?).map_err(|_| invalid_token())?;
        let key = Key::try_from(Bytes::copy_from_slice(reader.read_exact(key_len)?))?;
        reader.finish()?;
        if key < request.range.start || key >= request.range.end {
            return Err(invalid_token());
        }
        Ok(key)
    }
}

impl TryFrom<Bytes> for ContinuationToken {
    type Error = StateStoreError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ChangeCursor(Bytes);

impl ChangeCursor {
    pub fn new(
        store_id: Uuid,
        revision: StoreRevision,
        sequence: u32,
    ) -> Result<Self, StateStoreError> {
        let revision_len =
            u32::try_from(revision.as_bytes().len()).map_err(|_| invalid_cursor())?;
        let capacity = 1usize
            .checked_add(16)
            .and_then(|size| size.checked_add(4))
            .and_then(|size| size.checked_add(revision.as_bytes().len()))
            .and_then(|size| size.checked_add(4))
            .ok_or_else(invalid_cursor)?;
        let mut encoded = BytesMut::with_capacity(capacity);
        encoded.put_u8(CODEC_VERSION);
        encoded.extend_from_slice(store_id.as_bytes());
        encoded.put_u32(revision_len);
        encoded.extend_from_slice(revision.as_bytes());
        encoded.put_u32(sequence);
        Ok(Self(encoded.freeze()))
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
    pub fn decode(&self, expected_store_id: Uuid) -> Result<(StoreRevision, u32), StateStoreError> {
        let mut reader = CheckedReader::new(self.as_bytes(), invalid_cursor);
        if reader.read_u8()? != CODEC_VERSION {
            return Err(invalid_cursor());
        }
        if reader.read_exact(16)? != expected_store_id.as_bytes() {
            return Err(invalid_cursor());
        }
        let revision_len = usize::try_from(reader.read_u32()?).map_err(|_| invalid_cursor())?;
        let revision =
            StoreRevision::try_from(Bytes::copy_from_slice(reader.read_exact(revision_len)?))?;
        let sequence = reader.read_u32()?;
        reader.finish()?;
        Ok((revision, sequence))
    }
}

impl TryFrom<Bytes> for ChangeCursor {
    type Error = StateStoreError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Ok(Self(value))
    }
}

fn request_fingerprint(
    range: &KeyRange,
    direction: Direction,
) -> Result<[u8; REQUEST_FINGERPRINT_BYTES], StateStoreError> {
    let start_len = u32::try_from(range.start.as_bytes().len()).map_err(|_| invalid_token())?;
    let end_len = u32::try_from(range.end.as_bytes().len()).map_err(|_| invalid_token())?;
    let mut hasher = Sha256::new();
    hasher.update([CODEC_VERSION]);
    hasher.update([direction.code()]);
    hasher.update(start_len.to_be_bytes());
    hasher.update(range.start.as_bytes());
    hasher.update(end_len.to_be_bytes());
    hasher.update(range.end.as_bytes());
    Ok(hasher.finalize().into())
}

struct CheckedReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    invalid: fn() -> StateStoreError,
}
impl<'a> CheckedReader<'a> {
    const fn new(bytes: &'a [u8], invalid: fn() -> StateStoreError) -> Self {
        Self {
            bytes,
            offset: 0,
            invalid,
        }
    }
    fn read_u8(&mut self) -> Result<u8, StateStoreError> {
        let bytes = self.read_exact(1)?;
        bytes.first().copied().ok_or_else(self.invalid)
    }
    fn read_u32(&mut self) -> Result<u32, StateStoreError> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| (self.invalid)())?;
        Ok(u32::from_be_bytes(bytes))
    }
    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], StateStoreError> {
        let end = self.offset.checked_add(length).ok_or_else(self.invalid)?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(self.invalid)?;
        self.offset = end;
        Ok(bytes)
    }
    fn finish(self) -> Result<(), StateStoreError> {
        if self.offset != self.bytes.len() {
            return Err((self.invalid)());
        }
        Ok(())
    }
}

const fn invalid_token() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::InvalidRequest,
        "invalid continuation token",
    )
}
const fn invalid_cursor() -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::InvalidRequest, "invalid change cursor")
}
