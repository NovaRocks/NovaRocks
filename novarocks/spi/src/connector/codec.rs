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

//! Provider-neutral connector codec contracts.
//!
//! This module deliberately contains no protobuf or application types. A wire
//! adapter validates the public envelope first, then gives the private payload
//! and this bounded context to the exact registered provider codec.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;

use super::{CatalogHandle, ConnectorProviderId};

pub const MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH: usize = 64;
pub const MAX_CONNECTOR_CODEC_FIELD_NAME_BYTES: usize = 256;
pub const MAX_CONNECTOR_CODEC_ERROR_DETAIL_BYTES: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConnectorCodecCategory {
    ReadTable,
    ReadView,
    ReadColumn,
    ReadSplit,
    WriteHandle,
    CommitFragment,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorCodecRevision(u32);

impl ConnectorCodecRevision {
    pub fn try_new(value: u32) -> Result<Self, ConnectorCodecError> {
        if value == 0 {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("codec_revision"),
                ConnectorCodecErrorKind::VersionMismatch,
                "connector codec revision must be non-zero",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorFieldPathSegment {
    Field(Arc<str>),
    Index(usize),
    MapKey(Arc<str>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorFieldPath(Arc<[ConnectorFieldPathSegment]>);

impl ConnectorFieldPath {
    pub fn root(name: impl AsRef<str>) -> Self {
        Self(Arc::from([ConnectorFieldPathSegment::Field(Arc::from(
            name.as_ref(),
        ))]))
    }

    pub fn field(&self, name: impl AsRef<str>) -> Self {
        self.push(ConnectorFieldPathSegment::Field(Arc::from(name.as_ref())))
    }

    pub fn index(&self, index: usize) -> Self {
        self.push(ConnectorFieldPathSegment::Index(index))
    }

    pub fn map_key(&self, key: impl AsRef<str>) -> Self {
        self.push(ConnectorFieldPathSegment::MapKey(Arc::from(key.as_ref())))
    }

    pub fn segments(&self) -> &[ConnectorFieldPathSegment] {
        &self.0
    }

    fn push(&self, segment: ConnectorFieldPathSegment) -> Self {
        let mut segments = self.0.to_vec();
        segments.push(segment);
        Self(Arc::from(segments))
    }

    fn is_bounded(&self) -> bool {
        self.0.len() <= MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH
            && self.0.iter().all(|segment| match segment {
                ConnectorFieldPathSegment::Field(value)
                | ConnectorFieldPathSegment::MapKey(value) => {
                    !value.is_empty() && value.len() <= MAX_CONNECTOR_CODEC_FIELD_NAME_BYTES
                }
                ConnectorFieldPathSegment::Index(_) => true,
            })
    }
}

impl fmt::Display for ConnectorFieldPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, segment) in self.0.iter().enumerate() {
            match (index, segment) {
                (0, ConnectorFieldPathSegment::Field(value)) => formatter.write_str(value)?,
                (_, ConnectorFieldPathSegment::Field(value)) => write!(formatter, ".{value}")?,
                (_, ConnectorFieldPathSegment::Index(value)) => write!(formatter, "[{value}]")?,
                (_, ConnectorFieldPathSegment::MapKey(value)) => write!(formatter, "[{value:?}]")?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorCodecErrorKind {
    MissingField,
    InvalidEnum,
    InvalidValue,
    DuplicateField,
    UnknownField,
    InconsistentFields,
    Unsupported,
    Capacity,
    VersionMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCodecError {
    path: ConnectorFieldPath,
    kind: ConnectorCodecErrorKind,
    detail: Arc<str>,
}

impl ConnectorCodecError {
    pub fn new(
        path: ConnectorFieldPath,
        kind: ConnectorCodecErrorKind,
        detail: impl AsRef<str>,
    ) -> Self {
        let path = if path.is_bounded() {
            path
        } else {
            ConnectorFieldPath::root("connector_payload")
        };
        Self {
            path,
            kind,
            detail: Arc::from(bound_detail(detail.as_ref())),
        }
    }

    pub const fn path(&self) -> &ConnectorFieldPath {
        &self.path
    }

    pub const fn kind(&self) -> ConnectorCodecErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for ConnectorCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "connector codec error at {} ({:?}): {}",
            self.path, self.kind, self.detail
        )
    }
}

impl Error for ConnectorCodecError {}

fn bound_detail(detail: &str) -> String {
    let mut value = detail.to_owned();
    for marker in ["password=", "secret=", "token="] {
        let mut offset = 0;
        while let Some(relative) = value[offset..].find(marker) {
            let start = offset + relative + marker.len();
            let end = value[start..]
                .find(char::is_whitespace)
                .map_or(value.len(), |relative| start + relative);
            value.replace_range(start..end, "[REDACTED]");
            offset = start + "[REDACTED]".len();
        }
    }
    if value.len() > MAX_CONNECTOR_CODEC_ERROR_DETAIL_BYTES {
        let mut end = MAX_CONNECTOR_CODEC_ERROR_DETAIL_BYTES;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
    }
    value
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorDecodeLimits {
    pub max_raw_bytes: usize,
    pub max_retained_bytes: usize,
    pub max_scalar_bytes: usize,
    pub max_items: usize,
    pub max_depth: usize,
}

impl ConnectorDecodeLimits {
    pub fn try_new(
        max_raw_bytes: usize,
        max_retained_bytes: usize,
        max_scalar_bytes: usize,
        max_items: usize,
        max_depth: usize,
    ) -> Result<Self, ConnectorCodecError> {
        if max_raw_bytes == 0
            || max_retained_bytes == 0
            || max_scalar_bytes == 0
            || max_items == 0
            || max_depth == 0
            || max_depth > MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH
        {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("decode_limits"),
                ConnectorCodecErrorKind::InvalidValue,
                "connector decode limits must be finite and non-zero",
            ));
        }
        Ok(Self {
            max_raw_bytes,
            max_retained_bytes,
            max_scalar_bytes,
            max_items,
            max_depth,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorDecodeLedger {
    limits: ConnectorDecodeLimits,
    raw_bytes: usize,
    retained_bytes: usize,
    scalar_bytes: usize,
    items: usize,
    depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorDecodeCheckpoint {
    raw_bytes: usize,
    retained_bytes: usize,
    scalar_bytes: usize,
    items: usize,
    depth: usize,
}

#[derive(Debug)]
pub struct ConnectorDecodeDepthGuard<'a> {
    ledger: &'a mut ConnectorDecodeLedger,
}

impl ConnectorDecodeDepthGuard<'_> {
    pub fn ledger(&mut self) -> &mut ConnectorDecodeLedger {
        self.ledger
    }
}

impl Drop for ConnectorDecodeDepthGuard<'_> {
    fn drop(&mut self) {
        self.ledger.depth -= 1;
    }
}

/// Immutable binding facts plus the caller-owned structural budget available
/// to one provider-private decoder. It exposes no request, credential, I/O or
/// runtime resource capability.
pub struct ConnectorDecodeContext<'a> {
    expected_header: &'a ConnectorEnvelopeHeader,
    ledger: &'a mut ConnectorDecodeLedger,
}

impl<'a> ConnectorDecodeContext<'a> {
    pub fn new(
        expected_header: &'a ConnectorEnvelopeHeader,
        ledger: &'a mut ConnectorDecodeLedger,
    ) -> Self {
        Self {
            expected_header,
            ledger,
        }
    }

    pub const fn expected_header(&self) -> &ConnectorEnvelopeHeader {
        self.expected_header
    }

    pub fn ledger(&mut self) -> &mut ConnectorDecodeLedger {
        self.ledger
    }

    pub fn validate_header(
        &self,
        actual: &ConnectorEnvelopeHeader,
    ) -> Result<(), ConnectorCodecError> {
        actual.validate_expected(
            self.expected_header.provider_id(),
            self.expected_header.catalog(),
            self.expected_header.category(),
            self.expected_header.codec_revision(),
        )
    }
}

/// A provider-owned pure encoder for one concrete value category.
pub trait ConnectorPrivateEncoder<T>: Send + Sync {
    fn encode_private(&self, value: &T) -> Result<Bytes, ConnectorCodecError>;
}

/// A provider-owned pure decoder for one concrete value category.
pub trait ConnectorPrivateDecoder<T>: Send + Sync {
    fn decode_private(
        &self,
        payload: &[u8],
        context: &mut ConnectorDecodeContext<'_>,
    ) -> Result<T, ConnectorCodecError>;
}

impl ConnectorDecodeLedger {
    pub const fn new(limits: ConnectorDecodeLimits) -> Self {
        Self {
            limits,
            raw_bytes: 0,
            retained_bytes: 0,
            scalar_bytes: 0,
            items: 0,
            depth: 0,
        }
    }

    pub fn charge_raw(&mut self, bytes: usize) -> Result<(), ConnectorCodecError> {
        charge(
            &mut self.raw_bytes,
            bytes,
            self.limits.max_raw_bytes,
            "raw bytes",
        )
    }

    pub fn charge_retained(&mut self, bytes: usize) -> Result<(), ConnectorCodecError> {
        charge(
            &mut self.retained_bytes,
            bytes,
            self.limits.max_retained_bytes,
            "retained bytes",
        )
    }

    pub fn charge_items(&mut self, items: usize) -> Result<(), ConnectorCodecError> {
        charge(&mut self.items, items, self.limits.max_items, "items")
    }

    pub fn charge_scalar(&mut self, bytes: usize) -> Result<(), ConnectorCodecError> {
        charge(
            &mut self.scalar_bytes,
            bytes,
            self.limits.max_scalar_bytes,
            "scalar bytes",
        )
    }

    pub fn check_depth(&self, depth: usize) -> Result<(), ConnectorCodecError> {
        if depth > self.limits.max_depth {
            return Err(capacity("nesting depth"));
        }
        Ok(())
    }

    pub fn enter_depth(&mut self) -> Result<ConnectorDecodeDepthGuard<'_>, ConnectorCodecError> {
        self.check_depth(self.depth + 1)?;
        self.depth += 1;
        Ok(ConnectorDecodeDepthGuard { ledger: self })
    }

    pub const fn checkpoint(&self) -> ConnectorDecodeCheckpoint {
        ConnectorDecodeCheckpoint {
            raw_bytes: self.raw_bytes,
            retained_bytes: self.retained_bytes,
            scalar_bytes: self.scalar_bytes,
            items: self.items,
            depth: self.depth,
        }
    }

    pub fn rollback(&mut self, checkpoint: ConnectorDecodeCheckpoint) {
        self.raw_bytes = checkpoint.raw_bytes.min(self.raw_bytes);
        self.retained_bytes = checkpoint.retained_bytes.min(self.retained_bytes);
        self.scalar_bytes = checkpoint.scalar_bytes.min(self.scalar_bytes);
        self.items = checkpoint.items.min(self.items);
        self.depth = checkpoint.depth.min(self.depth);
    }

    pub const fn raw_bytes(&self) -> usize {
        self.raw_bytes
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub const fn items(&self) -> usize {
        self.items
    }

    pub const fn scalar_bytes(&self) -> usize {
        self.scalar_bytes
    }
}

fn charge(
    current: &mut usize,
    amount: usize,
    maximum: usize,
    subject: &'static str,
) -> Result<(), ConnectorCodecError> {
    let next = current
        .checked_add(amount)
        .ok_or_else(|| capacity(subject))?;
    if next > maximum {
        return Err(capacity(subject));
    }
    *current = next;
    Ok(())
}

fn capacity(subject: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root("connector_payload"),
        ConnectorCodecErrorKind::Capacity,
        format!("connector {subject} exceed the decode budget"),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorEnvelopeHeader {
    provider_id: ConnectorProviderId,
    catalog: CatalogHandle,
    category: ConnectorCodecCategory,
    codec_revision: ConnectorCodecRevision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorEncodedPayload {
    header: ConnectorEnvelopeHeader,
    payload: Bytes,
}

impl ConnectorEncodedPayload {
    pub fn new(header: ConnectorEnvelopeHeader, payload: Bytes) -> Self {
        Self { header, payload }
    }

    pub const fn header(&self) -> &ConnectorEnvelopeHeader {
        &self.header
    }

    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    pub fn into_parts(self) -> (ConnectorEnvelopeHeader, Bytes) {
        (self.header, self.payload)
    }
}

impl ConnectorEnvelopeHeader {
    pub const fn new(
        provider_id: ConnectorProviderId,
        catalog: CatalogHandle,
        category: ConnectorCodecCategory,
        codec_revision: ConnectorCodecRevision,
    ) -> Self {
        Self {
            provider_id,
            catalog,
            category,
            codec_revision,
        }
    }

    pub const fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub const fn catalog(&self) -> &CatalogHandle {
        &self.catalog
    }

    pub const fn category(&self) -> ConnectorCodecCategory {
        self.category
    }

    pub const fn codec_revision(&self) -> ConnectorCodecRevision {
        self.codec_revision
    }

    pub fn validate_expected(
        &self,
        provider_id: &ConnectorProviderId,
        catalog: &CatalogHandle,
        category: ConnectorCodecCategory,
        codec_revision: ConnectorCodecRevision,
    ) -> Result<(), ConnectorCodecError> {
        if &self.provider_id != provider_id {
            return Err(mismatch("provider_id"));
        }
        if &self.catalog != catalog {
            return Err(mismatch("catalog"));
        }
        if self.category != category {
            return Err(mismatch("category"));
        }
        if self.codec_revision != codec_revision {
            return Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("header").field("codec_revision"),
                ConnectorCodecErrorKind::VersionMismatch,
                "connector codec revision does not match the installed definition",
            ));
        }
        Ok(())
    }
}

fn mismatch(field: &'static str) -> ConnectorCodecError {
    ConnectorCodecError::new(
        ConnectorFieldPath::root("header").field(field),
        ConnectorCodecErrorKind::InconsistentFields,
        "connector envelope header does not match the installed binding",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{CatalogVersion, ConnectorInstanceId};

    fn header(category: ConnectorCodecCategory, revision: u32) -> ConnectorEnvelopeHeader {
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([7; 32]),
            ),
            category,
            ConnectorCodecRevision::try_new(revision).unwrap(),
        )
    }

    #[test]
    fn decode_ledger_keeps_raw_retained_item_and_depth_limits_independent() {
        let limits = ConnectorDecodeLimits::try_new(8, 16, 6, 2, 3).unwrap();
        let mut ledger = ConnectorDecodeLedger::new(limits);
        ledger.charge_raw(8).unwrap();
        ledger.charge_retained(16).unwrap();
        ledger.charge_items(2).unwrap();
        ledger.charge_scalar(6).unwrap();
        ledger.check_depth(3).unwrap();
        assert_eq!(ledger.raw_bytes(), 8);
        assert_eq!(ledger.retained_bytes(), 16);
        assert_eq!(ledger.items(), 2);
        assert_eq!(ledger.scalar_bytes(), 6);
        assert_eq!(
            ledger.charge_raw(1).unwrap_err().kind(),
            ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.charge_retained(1).unwrap_err().kind(),
            ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.charge_items(1).unwrap_err().kind(),
            ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.charge_scalar(1).unwrap_err().kind(),
            ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.check_depth(4).unwrap_err().kind(),
            ConnectorCodecErrorKind::Capacity
        );
    }

    #[test]
    fn refused_charge_does_not_mutate_the_ledger() {
        let limits = ConnectorDecodeLimits::try_new(8, 8, 8, 8, 8).unwrap();
        let mut ledger = ConnectorDecodeLedger::new(limits);
        ledger.charge_raw(7).unwrap();
        let checkpoint = ledger.checkpoint();
        assert!(ledger.charge_raw(2).is_err());
        assert_eq!(ledger.raw_bytes(), 7);
        ledger.charge_scalar(4).unwrap();
        ledger.rollback(checkpoint);
        assert_eq!(ledger.scalar_bytes(), 0);
    }

    #[test]
    fn envelope_header_requires_exact_binding_category_and_revision() {
        let expected = header(ConnectorCodecCategory::ReadSplit, 2);
        expected
            .validate_expected(
                expected.provider_id(),
                expected.catalog(),
                ConnectorCodecCategory::ReadSplit,
                ConnectorCodecRevision::try_new(2).unwrap(),
            )
            .unwrap();

        let wrong_catalog = CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("lake").unwrap(),
            CatalogVersion::from_bytes([8; 32]),
        );
        assert_eq!(
            expected
                .validate_expected(
                    expected.provider_id(),
                    &wrong_catalog,
                    ConnectorCodecCategory::ReadSplit,
                    ConnectorCodecRevision::try_new(2).unwrap(),
                )
                .unwrap_err()
                .kind(),
            ConnectorCodecErrorKind::InconsistentFields
        );
        assert_eq!(
            expected
                .validate_expected(
                    expected.provider_id(),
                    expected.catalog(),
                    ConnectorCodecCategory::ReadTable,
                    ConnectorCodecRevision::try_new(2).unwrap(),
                )
                .unwrap_err()
                .kind(),
            ConnectorCodecErrorKind::InconsistentFields
        );
        assert_eq!(
            expected
                .validate_expected(
                    expected.provider_id(),
                    expected.catalog(),
                    ConnectorCodecCategory::ReadSplit,
                    ConnectorCodecRevision::try_new(3).unwrap(),
                )
                .unwrap_err()
                .kind(),
            ConnectorCodecErrorKind::VersionMismatch
        );
    }

    #[test]
    fn codec_revision_and_limits_are_finite() {
        assert!(ConnectorCodecRevision::try_new(0).is_err());
        assert!(ConnectorDecodeLimits::try_new(0, 1, 1, 1, 1).is_err());
        assert!(ConnectorDecodeLimits::try_new(1, 1, 1, 1, 65).is_err());
    }

    #[test]
    fn field_paths_are_owned_and_bounded() {
        let path = ConnectorFieldPath::root("relation")
            .field("columns")
            .index(2)
            .map_key("field-id");
        assert_eq!(path.to_string(), "relation.columns[2][\"field-id\"]");
        let mut too_deep = ConnectorFieldPath::root("root");
        for _ in 0..MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH {
            too_deep = too_deep.field("child");
        }
        let error =
            ConnectorCodecError::new(too_deep, ConnectorCodecErrorKind::InvalidValue, "bad");
        assert_eq!(error.path().to_string(), "connector_payload");
    }

    #[test]
    fn depth_guard_releases_its_level_and_errors_are_redacted_and_bounded() {
        let limits = ConnectorDecodeLimits::try_new(8, 8, 8, 8, 1).unwrap();
        let mut ledger = ConnectorDecodeLedger::new(limits);
        {
            let mut level = ledger.enter_depth().unwrap();
            assert_eq!(
                level.ledger().enter_depth().unwrap_err().kind(),
                ConnectorCodecErrorKind::Capacity
            );
        }
        ledger.enter_depth().unwrap();

        let error = ConnectorCodecError::new(
            ConnectorFieldPath::root("payload"),
            ConnectorCodecErrorKind::InvalidValue,
            format!("password=canary {}", "x".repeat(1024)),
        );
        assert!(!error.detail().contains("canary"));
        assert!(error.detail().contains("password=[REDACTED]"));
        assert!(error.detail().len() <= MAX_CONNECTOR_CODEC_ERROR_DETAIL_BYTES);
    }
}
