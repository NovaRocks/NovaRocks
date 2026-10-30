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
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use super::read_stack::{
    ConnectorReadColumnHandle, ConnectorReadRelation, ConnectorReadSplit, ConnectorReadSplitFacts,
    ConnectorReadTransactionHandle,
};
use super::write_stack::{ConnectorCommitFragment, ConnectorWriterHandle};

pub use novarocks_connector_contract::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorReadRelationPayload,
};

pub const MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH: usize = 64;
pub const MAX_CONNECTOR_CODEC_FIELD_NAME_BYTES: usize = 256;
pub const MAX_CONNECTOR_CODEC_ERROR_DETAIL_BYTES: usize = 512;

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

#[derive(Clone, Debug, Eq, PartialEq)]
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
    CompileControl(CompileControlError),
    Domain(novarocks_failure_contract::SharedFailureCause),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCodecError {
    path: ConnectorFieldPath,
    kind: ConnectorCodecErrorKind,
    detail: Arc<str>,
    resource: Option<novarocks_failure_contract::ResourceFacts>,
    contract: Option<ConnectorCodecContractError>,
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
            resource: None,
            contract: None,
        }
    }

    /// Add codec location while retaining the producer's exact failure facts.
    pub fn from_domain<E>(path: ConnectorFieldPath, cause: E) -> Self
    where
        E: Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
    {
        let source = novarocks_failure_contract::SharedFailureCause::new(cause);
        let detail = source.detail().to_owned();
        Self::new(path, ConnectorCodecErrorKind::Domain(source), detail)
    }

    pub const fn path(&self) -> &ConnectorFieldPath {
        &self.path
    }

    pub const fn kind(&self) -> &ConnectorCodecErrorKind {
        &self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// Preserve the exact caller control failure across codec diagnostic paths.
    pub const fn compile_control_error(&self) -> Option<&CompileControlError> {
        match &self.kind {
            ConnectorCodecErrorKind::CompileControl(error) => Some(error),
            _ => None,
        }
    }

    /// Attach an enclosing field path without changing a typed failure cause.
    pub fn with_path(mut self, path: ConnectorFieldPath) -> Self {
        self.path = if path.is_bounded() {
            path
        } else {
            ConnectorFieldPath::root("connector_payload")
        };
        self
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

impl Error for ConnectorCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ConnectorCodecErrorKind::CompileControl(error) => Some(error),
            ConnectorCodecErrorKind::Domain(source) => Some(source),
            _ => self
                .contract
                .as_ref()
                .map(|error| error as &(dyn Error + 'static)),
        }
    }
}

impl novarocks_failure_contract::FailureProjection for ConnectorCodecError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        if let Some(error) = self.compile_control_error() {
            return error.failure_fact();
        }
        if let Some(error) = self.contract.as_ref() {
            return error.failure_fact();
        }
        // Codec owns these format/support facts. Provider prose never selects
        // a class, quantity, stop intent, or retry permission.
        let (code, class) = match &self.kind {
            ConnectorCodecErrorKind::MissingField => (160, FailureClass::Invariant),
            ConnectorCodecErrorKind::InvalidEnum => (161, FailureClass::Invariant),
            ConnectorCodecErrorKind::InvalidValue => (162, FailureClass::Invariant),
            ConnectorCodecErrorKind::DuplicateField => (163, FailureClass::Invariant),
            ConnectorCodecErrorKind::UnknownField => (164, FailureClass::Invariant),
            ConnectorCodecErrorKind::InconsistentFields => (165, FailureClass::Invariant),
            ConnectorCodecErrorKind::Unsupported => (166, FailureClass::Unsupported),
            ConnectorCodecErrorKind::Capacity => (167, FailureClass::Resource),
            ConnectorCodecErrorKind::VersionMismatch => (168, FailureClass::Invariant),
            ConnectorCodecErrorKind::CompileControl(error) => return error.failure_fact(),
            ConnectorCodecErrorKind::Domain(source) => return source.failure_fact(),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(code).unwrap()),
        );
        self.resource
            .map_or(fact, |resource| fact.with_resource(resource))
    }
}

impl From<CompileControlError> for ConnectorCodecError {
    fn from(error: CompileControlError) -> Self {
        let detail = Arc::from(error.detail());
        Self {
            path: ConnectorFieldPath::root("connector_payload"),
            kind: ConnectorCodecErrorKind::CompileControl(error),
            detail,
            resource: None,
            contract: None,
        }
    }
}

impl From<ConnectorCodecContractError> for ConnectorCodecError {
    fn from(error: ConnectorCodecContractError) -> Self {
        let (path, kind) = match error {
            ConnectorCodecContractError::RevisionMustBeNonZero => (
                ConnectorFieldPath::root("codec_revision"),
                ConnectorCodecErrorKind::VersionMismatch,
            ),
            ConnectorCodecContractError::ProviderMismatch => (
                ConnectorFieldPath::root("header").field("provider_id"),
                ConnectorCodecErrorKind::InconsistentFields,
            ),
            ConnectorCodecContractError::CatalogMismatch => (
                ConnectorFieldPath::root("header").field("catalog"),
                ConnectorCodecErrorKind::InconsistentFields,
            ),
            ConnectorCodecContractError::CategoryMismatch => (
                ConnectorFieldPath::root("header").field("category"),
                ConnectorCodecErrorKind::InconsistentFields,
            ),
            ConnectorCodecContractError::RevisionMismatch => (
                ConnectorFieldPath::root("header").field("codec_revision"),
                ConnectorCodecErrorKind::VersionMismatch,
            ),
        };
        let mut result = Self::new(path, kind, error.to_string());
        result.contract = Some(error);
        result
    }
}

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
    compile_checkpoints: Option<CompileCheckpoints<'a>>,
}

impl<'a> ConnectorDecodeContext<'a> {
    pub fn new(
        expected_header: &'a ConnectorEnvelopeHeader,
        ledger: &'a mut ConnectorDecodeLedger,
    ) -> Self {
        Self {
            expected_header,
            ledger,
            compile_checkpoints: None,
        }
    }

    /// Pure preparation borrows the original caller control; the decoded value
    /// retains no control object. Structural byte limits remain independent.
    pub fn try_new_for_compile(
        expected_header: &'a ConnectorEnvelopeHeader,
        ledger: &'a mut ConnectorDecodeLedger,
        control: &'a dyn PureCompileControl,
    ) -> Result<Self, ConnectorCodecError> {
        let checkpoints = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        Ok(Self {
            expected_header,
            ledger,
            compile_checkpoints: Some(checkpoints),
        })
    }

    pub const fn is_compile_observed(&self) -> bool {
        self.compile_checkpoints.is_some()
    }

    /// Account one completed bounded decoder operation. Legacy runtime decoding
    /// has no compile scope; pure preparation must use try_new_for_compile.
    pub fn observe_compile_step(&mut self) -> Result<(), ConnectorCodecError> {
        if let Some(checkpoints) = &mut self.compile_checkpoints {
            checkpoints.step()?;
        }
        Ok(())
    }

    /// Observe the tail before handing work or values to another owner. Opaque
    /// library calls still require finite input bounds of their own.
    pub fn flush_compile_control(&mut self) -> Result<(), ConnectorCodecError> {
        if let Some(checkpoints) = &mut self.compile_checkpoints {
            checkpoints.flush()?;
        }
        Ok(())
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

/// Public scheduling category paired with one provider-private split payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadSplitCategory {
    Data,
    TableChanges,
    ChangeWindow,
    SystemManifest,
    SystemRelation,
    RewritePositionDeleteFiles,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadSplitPayload {
    category: ConnectorReadSplitCategory,
    provider_payload: ConnectorEncodedPayload,
}

impl ConnectorReadSplitPayload {
    pub const fn new(
        category: ConnectorReadSplitCategory,
        provider_payload: ConnectorEncodedPayload,
    ) -> Self {
        Self {
            category,
            provider_payload,
        }
    }

    pub const fn category(&self) -> ConnectorReadSplitCategory {
        self.category
    }

    pub const fn provider_payload(&self) -> &ConnectorEncodedPayload {
        &self.provider_payload
    }
}

/// FE-facing provider codec facet. It produces only SPI payloads; generated
/// public DTOs remain the responsibility of the outer wire adapter.
pub trait ConnectorReadWireEncoder: Send + Sync {
    fn owner(&self) -> &str;

    fn encode_relation_payload(
        &self,
        relation: &ConnectorReadRelation,
    ) -> Result<ConnectorReadRelationPayload, ConnectorCodecError>;

    fn encode_column_payload(
        &self,
        column: &ConnectorReadColumnHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError>;

    fn encode_transaction_payload(
        &self,
        transaction: &ConnectorReadTransactionHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError>;

    fn encode_split_payload(
        &self,
        split: &ConnectorReadSplit,
    ) -> Result<ConnectorReadSplitPayload, ConnectorCodecError>;
}

/// BE-facing provider codec facet. Public protobuf validation has completed
/// before these methods receive an SPI payload.
pub trait ConnectorReadWireDecoder: Send + Sync {
    fn owner(&self) -> &str;

    fn decode_relation_payload(
        &self,
        payload: &ConnectorReadRelationPayload,
    ) -> Result<ConnectorReadRelation, ConnectorCodecError>;

    fn decode_column_payload(
        &self,
        payload: &ConnectorEncodedPayload,
    ) -> Result<ConnectorReadColumnHandle, ConnectorCodecError>;

    fn decode_transaction_payload(
        &self,
        payload: &ConnectorEncodedPayload,
    ) -> Result<ConnectorReadTransactionHandle, ConnectorCodecError>;

    fn decode_split_payload(
        &self,
        payload: &ConnectorReadSplitPayload,
        facts: &ConnectorReadSplitFacts,
    ) -> Result<ConnectorReadSplit, ConnectorCodecError>;
}

pub trait ConnectorWriteHandleWireEncoder: Send + Sync {
    fn owner(&self) -> &str;

    fn encode_writer_handle_payload(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError>;
}

pub trait ConnectorWriteHandleWireDecoder: Send + Sync {
    fn owner(&self) -> &str;

    fn decode_writer_handle_payload(
        &self,
        payload: &ConnectorEncodedPayload,
    ) -> Result<ConnectorWriterHandle, ConnectorCodecError>;
}

pub trait ConnectorWriteFragmentWireEncoder: Send + Sync {
    fn owner(&self) -> &str;

    fn encode_commit_fragment_payload(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<ConnectorEncodedPayload, ConnectorCodecError>;
}

pub trait ConnectorWriteFragmentWireDecoder: Send + Sync {
    fn owner(&self) -> &str;

    fn decode_commit_fragment_payload(
        &self,
        payload: &ConnectorEncodedPayload,
    ) -> Result<ConnectorCommitFragment, ConnectorCodecError>;
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
            novarocks_failure_contract::ResourceDimension::Bytes,
        )
    }

    pub fn charge_retained(&mut self, bytes: usize) -> Result<(), ConnectorCodecError> {
        charge(
            &mut self.retained_bytes,
            bytes,
            self.limits.max_retained_bytes,
            "retained bytes",
            novarocks_failure_contract::ResourceDimension::Bytes,
        )
    }

    pub fn charge_items(&mut self, items: usize) -> Result<(), ConnectorCodecError> {
        charge(
            &mut self.items,
            items,
            self.limits.max_items,
            "items",
            novarocks_failure_contract::ResourceDimension::Items,
        )
    }

    pub fn charge_scalar(&mut self, bytes: usize) -> Result<(), ConnectorCodecError> {
        charge(
            &mut self.scalar_bytes,
            bytes,
            self.limits.max_scalar_bytes,
            "scalar bytes",
            novarocks_failure_contract::ResourceDimension::Bytes,
        )
    }

    pub fn check_depth(&self, depth: usize) -> Result<(), ConnectorCodecError> {
        if depth > self.limits.max_depth {
            let mut error = capacity(
                "nesting depth",
                novarocks_failure_contract::ResourceDimension::Positions,
                depth,
                0,
                self.limits.max_depth,
            );
            // This API checks an absolute depth, not a charge against the
            // current ledger depth. Do not invent committed/growth values.
            if let Some(resource) = &mut error.resource {
                resource.committed = None;
                resource.required_growth = None;
            }
            return Err(error);
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
    dimension: novarocks_failure_contract::ResourceDimension,
) -> Result<(), ConnectorCodecError> {
    let next = current
        .checked_add(amount)
        .ok_or_else(|| capacity(subject, dimension, amount, *current, maximum))?;
    if next > maximum {
        return Err(capacity(subject, dimension, amount, *current, maximum));
    }
    *current = next;
    Ok(())
}

fn capacity(
    subject: &'static str,
    dimension: novarocks_failure_contract::ResourceDimension,
    requested: usize,
    committed: usize,
    limit: usize,
) -> ConnectorCodecError {
    use novarocks_failure_contract::{ResourceFacts, ResourceScope};
    let mut error = ConnectorCodecError::new(
        ConnectorFieldPath::root("connector_payload"),
        ConnectorCodecErrorKind::Capacity,
        format!("connector {subject} exceed the decode budget"),
    );
    error.resource = Some(ResourceFacts {
        request_scope: Some(ResourceScope::Request),
        constraint_scope: Some(ResourceScope::Request),
        dimension,
        requested: u64::try_from(requested).ok(),
        required_growth: u64::try_from(requested).ok(),
        limit: u64::try_from(limit).ok(),
        live: None,
        committed: u64::try_from(committed).ok(),
    });
    error
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
    };

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
            &ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.charge_retained(1).unwrap_err().kind(),
            &ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.charge_items(1).unwrap_err().kind(),
            &ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.charge_scalar(1).unwrap_err().kind(),
            &ConnectorCodecErrorKind::Capacity
        );
        assert_eq!(
            ledger.check_depth(4).unwrap_err().kind(),
            &ConnectorCodecErrorKind::Capacity
        );
    }

    #[test]
    fn refused_charge_does_not_mutate_the_ledger() {
        let limits = ConnectorDecodeLimits::try_new(8, 8, 8, 8, 8).unwrap();
        let mut ledger = ConnectorDecodeLedger::new(limits);
        ledger.charge_raw(7).unwrap();
        let checkpoint = ledger.checkpoint();
        use novarocks_failure_contract::{
            FailureClass, FailureProjection, ResourceDimension, ResourceScope,
        };
        let refusal = ledger.charge_raw(2).expect_err("7 + 2 exceeds 8");
        let fact = refusal.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().expect("known ledger quantities");
        assert_eq!(resource.dimension, ResourceDimension::Bytes);
        assert_eq!(resource.request_scope, Some(ResourceScope::Request));
        assert_eq!(resource.constraint_scope, Some(ResourceScope::Request));
        assert_eq!(
            (
                resource.requested,
                resource.required_growth,
                resource.committed,
                resource.limit,
                resource.live
            ),
            (Some(2), Some(2), Some(7), Some(8), None)
        );
        assert_eq!(ledger.raw_bytes(), 7);
        let depth = ledger
            .check_depth(9)
            .expect_err("absolute depth exceeds 8")
            .failure_fact()
            .resource()
            .unwrap();
        assert_eq!(
            (depth.dimension, depth.requested, depth.limit),
            (ResourceDimension::Positions, Some(9), Some(8))
        );
        assert_eq!(
            (depth.committed, depth.required_growth, depth.live),
            (None, None, None)
        );
        ledger.charge_scalar(4).unwrap();
        ledger.rollback(checkpoint);
        assert_eq!(ledger.scalar_bytes(), 0);
    }

    #[test]
    fn envelope_header_requires_exact_binding_category_and_revision() {
        let expected = header(ConnectorCodecCategory::ReadSplit, 2);
        expected
            .validate_expected::<ConnectorCodecError>(
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
                .validate_expected::<ConnectorCodecError>(
                    expected.provider_id(),
                    &wrong_catalog,
                    ConnectorCodecCategory::ReadSplit,
                    ConnectorCodecRevision::try_new(2).unwrap(),
                )
                .unwrap_err()
                .kind(),
            &ConnectorCodecErrorKind::InconsistentFields
        );
        assert_eq!(
            expected
                .validate_expected::<ConnectorCodecError>(
                    expected.provider_id(),
                    expected.catalog(),
                    ConnectorCodecCategory::ReadTable,
                    ConnectorCodecRevision::try_new(2).unwrap(),
                )
                .unwrap_err()
                .kind(),
            &ConnectorCodecErrorKind::InconsistentFields
        );
        assert_eq!(
            expected
                .validate_expected::<ConnectorCodecError>(
                    expected.provider_id(),
                    expected.catalog(),
                    ConnectorCodecCategory::ReadSplit,
                    ConnectorCodecRevision::try_new(3).unwrap(),
                )
                .unwrap_err()
                .kind(),
            &ConnectorCodecErrorKind::VersionMismatch
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
                &ConnectorCodecErrorKind::Capacity
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

    #[test]
    fn codec_domain_keeps_exact_resource_fact_source_and_bounded_path_context() {
        use novarocks_failure_contract::{
            FailureProjection, ResourceDimension, SharedFailureCause,
        };
        let original = capacity("bytes", ResourceDimension::Bytes, 2, 7, 8);
        let fact = original.failure_fact();
        let codec = ConnectorCodecError::from_domain(
            ConnectorFieldPath::root("read_columns").index(17),
            original.clone(),
        );
        assert!(matches!(codec.kind(), ConnectorCodecErrorKind::Domain(_)));
        assert!(codec.compile_control_error().is_none());
        assert_eq!(codec.path().to_string(), "read_columns[17]");
        assert_eq!(codec.failure_fact(), fact);
        let resource = fact.resource().unwrap();
        assert_eq!(
            (
                resource.requested,
                resource.required_growth,
                resource.committed,
                resource.limit,
                resource.live
            ),
            (Some(2), Some(2), Some(7), Some(8), None)
        );
        let shared = codec
            .source()
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert_eq!(
            shared.downcast_ref::<ConnectorCodecError>(),
            Some(&original)
        );
        let moved = codec.clone().with_path(ConnectorFieldPath::root("outer"));
        assert_eq!(moved.failure_fact(), fact);
        assert_eq!(
            moved.source().unwrap().downcast_ref::<SharedFailureCause>(),
            Some(shared)
        );
        let unsupported = ConnectorCodecError::new(
            ConnectorFieldPath::root("capability"),
            ConnectorCodecErrorKind::Unsupported,
            "unavailable resource refused",
        );
        assert_eq!(
            unsupported.failure_fact().class(),
            novarocks_failure_contract::FailureClass::Unsupported
        );
        assert_eq!(unsupported.failure_fact().reason().code(), 166);
    }

    #[test]
    fn codec_projection_preserves_control_contract_and_unknown_provider_quantities() {
        use novarocks_failure_contract::{FailureClass, FailureEvidence, FailureProjection};
        for original in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let codec = ConnectorCodecError::from(original.clone())
                .with_path(ConnectorFieldPath::root("scan"));
            assert_eq!(codec.failure_fact(), original.failure_fact());
            assert_eq!(
                codec
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&original)
            );
        }
        let original = ConnectorCodecContractError::CatalogMismatch;
        let codec = ConnectorCodecError::from(original).with_path(ConnectorFieldPath::root("scan"));
        assert_eq!(codec.failure_fact(), original.failure_fact());
        assert_eq!(
            codec
                .source()
                .unwrap()
                .downcast_ref::<ConnectorCodecContractError>(),
            Some(&original)
        );
        let unknown = ConnectorCodecError::new(
            ConnectorFieldPath::root("provider"),
            ConnectorCodecErrorKind::Capacity,
            "cancelled deadline overflow",
        );
        assert_eq!(unknown.failure_fact().class(), FailureClass::Resource);
        assert_eq!(
            unknown.failure_fact().evidence(),
            FailureEvidence::Originating
        );
        assert_eq!(unknown.failure_fact().resource(), None);
        let invalid = ConnectorCodecError::new(
            ConnectorFieldPath::root("provider"),
            ConnectorCodecErrorKind::InvalidValue,
            "out of memory",
        );
        assert_eq!(invalid.failure_fact().class(), FailureClass::Invariant);
    }

    struct CompileOwner {
        calls: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
        failure_at: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for CompileOwner {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            if let Some((index, error)) = self.failure_at.clone()
                && calls.len() == index
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn compile_ledger() -> ConnectorDecodeLedger {
        ConnectorDecodeLedger::new(
            ConnectorDecodeLimits::try_new(1024, 1024, 1024, 1024, 8).unwrap(),
        )
    }

    #[test]
    fn compile_control_bridge_keeps_each_fixed_diagnostic_and_original_fact() {
        use novarocks_failure_contract::FailureProjection;
        for (cause, detail) in [
            (
                CompileControlError::Cancelled,
                "pure compilation was cancelled",
            ),
            (
                CompileControlError::DeadlineExceeded,
                "pure compilation deadline was exceeded",
            ),
            (
                CompileControlError::ResourceExhausted,
                "pure compilation work budget was exhausted",
            ),
        ] {
            let expected = cause.failure_fact();
            let error = ConnectorCodecError::from(cause.clone());
            assert_eq!(error.detail(), detail);
            assert_eq!(error.failure_fact(), expected);
            assert_eq!(error.path().to_string(), "connector_payload");
            assert_eq!(error.compile_control_error(), Some(&cause));
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }

    #[test]
    fn compile_control_source_bridge_keeps_the_bounded_snapshot_and_last_real_cause_holder() {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureProjection, FailureReason, SharedFailureCause, domain,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct Original {
            displays: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
        }
        impl fmt::Display for Original {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.displays.fetch_add(1, Ordering::SeqCst);
                f.write_str("provider token=original-value ")?;
                for _ in 0..1024 {
                    f.write_str("界")?;
                }
                Ok(())
            }
        }
        impl Error for Original {}
        impl FailureProjection for Original {
            fn failure_fact(&self) -> FailureFact {
                FailureFact::new(
                    FailureClass::Data,
                    FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(301).unwrap()),
                )
            }
        }
        impl Drop for Original {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }

        let displays = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let source = SharedFailureCause::new(Original {
            displays: displays.clone(),
            drops: drops.clone(),
        });
        let expected = source.detail().to_owned();
        let fact = source.failure_fact();
        let original = source.downcast_ref::<Original>().unwrap() as *const Original;
        let prefix = "provider token=original-value ";
        assert_eq!(
            expected.len(),
            prefix.len()
                + (novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES - prefix.len()) / "界".len()
                    * "界".len(),
        );
        let error = ConnectorCodecError::from(CompileControlError::Source(source.clone()));
        assert_eq!(error.detail(), expected);
        assert!(error.detail().starts_with(prefix));
        assert_eq!(error.failure_fact(), fact);
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(displays.load(Ordering::SeqCst), 1);
        match error.compile_control_error().unwrap() {
            CompileControlError::Source(held) => {
                assert_eq!(held, &source);
                assert_eq!(
                    held.downcast_ref::<Original>().unwrap() as *const Original,
                    original
                );
            }
            _ => panic!("the original source must remain typed"),
        }
        let clone = error
            .clone()
            .with_path(ConnectorFieldPath::root("fragment"));
        assert!(Arc::ptr_eq(&error.detail, &clone.detail));
        let diagnostic = Arc::downgrade(&error.detail);
        drop(source);
        drop(error);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(clone.detail(), expected);
        assert!(diagnostic.upgrade().is_some());
        drop(clone);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(diagnostic.upgrade().is_none());
        assert_eq!(displays.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn compile_decode_entry_refuses_each_control_cause_before_work() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::Source(novarocks_failure_contract::SharedFailureCause::new(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
            )),
        ] {
            let owner = CompileOwner {
                calls: Default::default(),
                failure_at: Some((1, cause.clone())),
            };
            let expected = header(ConnectorCodecCategory::ReadTable, 1);
            let mut ledger = compile_ledger();
            let error =
                match ConnectorDecodeContext::try_new_for_compile(&expected, &mut ledger, &owner) {
                    Ok(_) => panic!("entry control failure must refuse decoding"),
                    Err(error) => error,
                };
            assert_eq!(
                error.kind(),
                &ConnectorCodecErrorKind::CompileControl(cause.clone())
            );
            assert_eq!(error.compile_control_error(), Some(&cause));
            assert_eq!(ledger.raw_bytes(), 0);
            assert_eq!(ledger.items(), 0);
            assert_eq!(
                *owner.calls.lock().unwrap(),
                vec![(CompilePhase::ProviderValidation, 0)]
            );
        }
    }

    #[test]
    fn compile_decode_work_observes_bounded_intervals_and_exact_tail() {
        let owner = CompileOwner {
            calls: Default::default(),
            failure_at: None,
        };
        let expected = header(ConnectorCodecCategory::ReadTable, 1);
        let mut ledger = compile_ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut ledger, &owner).unwrap();
        for _ in 0..519 {
            context.ledger().charge_items(1).unwrap();
            context.observe_compile_step().unwrap();
        }
        context.flush_compile_control().unwrap();
        assert_eq!(context.ledger().items(), 519);
        assert_eq!(
            *owner.calls.lock().unwrap(),
            vec![
                (CompilePhase::ProviderValidation, 0),
                (CompilePhase::ProviderValidation, 256),
                (CompilePhase::ProviderValidation, 256),
                (CompilePhase::ProviderValidation, 7),
            ]
        );
    }

    #[test]
    fn compile_decode_failure_latches_and_ledger_rollback_does_not_reset_control() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let owner = CompileOwner {
                calls: Default::default(),
                failure_at: Some((2, cause.clone())),
            };
            let expected = header(ConnectorCodecCategory::ReadTable, 1);
            let mut ledger = compile_ledger();
            let mut context =
                ConnectorDecodeContext::try_new_for_compile(&expected, &mut ledger, &owner)
                    .unwrap();
            let structural_checkpoint = context.ledger().checkpoint();
            for _ in 0..255 {
                context.ledger().charge_items(1).unwrap();
                context.observe_compile_step().unwrap();
            }
            let first = context.observe_compile_step().unwrap_err();
            context.ledger().rollback(structural_checkpoint);
            assert_eq!(context.ledger().items(), 0);
            assert_eq!(
                context
                    .observe_compile_step()
                    .unwrap_err()
                    .compile_control_error(),
                Some(&cause)
            );
            assert_eq!(
                context
                    .flush_compile_control()
                    .unwrap_err()
                    .compile_control_error(),
                Some(&cause)
            );
            assert_eq!(first.compile_control_error(), Some(&cause));
            assert_eq!(
                *owner.calls.lock().unwrap(),
                vec![
                    (CompilePhase::ProviderValidation, 0),
                    (CompilePhase::ProviderValidation, 256),
                ]
            );
        }
    }

    #[test]
    fn compile_decode_tail_can_refuse_success_with_original_control_cause() {
        let cause = CompileControlError::DeadlineExceeded;
        let owner = CompileOwner {
            calls: Default::default(),
            failure_at: Some((2, cause.clone())),
        };
        let expected = header(ConnectorCodecCategory::ReadTable, 1);
        let mut ledger = compile_ledger();
        let mut context =
            ConnectorDecodeContext::try_new_for_compile(&expected, &mut ledger, &owner).unwrap();
        for _ in 0..3 {
            context.observe_compile_step().unwrap();
        }
        let error = context.flush_compile_control().unwrap_err();
        assert_eq!(error.compile_control_error(), Some(&cause));
        assert_eq!(
            *owner.calls.lock().unwrap(),
            vec![
                (CompilePhase::ProviderValidation, 0),
                (CompilePhase::ProviderValidation, 3),
            ]
        );
    }

    #[test]
    fn compile_codec_paths_keep_typed_cause_and_diagnostics_do_not_invent_one() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = ConnectorCodecError::from(cause.clone())
                .with_path(ConnectorFieldPath::root("columns").index(17));
            assert_eq!(error.path().to_string(), "columns[17]");
            assert_eq!(error.compile_control_error(), Some(&cause));
            let mut too_deep = ConnectorFieldPath::root("root");
            for _ in 0..MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH {
                too_deep = too_deep.field("child");
            }
            let bounded_error = error.clone().with_path(too_deep);
            assert_eq!(bounded_error.path().to_string(), "connector_payload");
            assert_eq!(bounded_error.compile_control_error(), Some(&cause));
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
        let diagnostic = ConnectorCodecError::new(
            ConnectorFieldPath::root("payload"),
            ConnectorCodecErrorKind::Capacity,
            "pure compilation was cancelled",
        );
        assert_eq!(diagnostic.compile_control_error(), None);
        assert!(diagnostic.source().is_none());
    }

    #[test]
    fn legacy_decode_context_keeps_structural_budget_without_compile_owner() {
        let expected = header(ConnectorCodecCategory::ReadTable, 1);
        let mut ledger = compile_ledger();
        let mut context = ConnectorDecodeContext::new(&expected, &mut ledger);
        assert!(!context.is_compile_observed());
        context.validate_header(&expected).unwrap();
        for _ in 0..519 {
            context.ledger().charge_items(1).unwrap();
            context.observe_compile_step().unwrap();
        }
        context.flush_compile_control().unwrap();
        assert_eq!(context.ledger().items(), 519);
        assert_eq!(context.expected_header(), &expected);
    }
}
