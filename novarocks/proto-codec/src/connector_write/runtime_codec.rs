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

//! The four directional write codec facets.
//!
//! The split is the point. An **encoder** can only create carrier values from
//! values its own provider already owns; it has no method that turns untrusted
//! carrier data into a provider value. A **decoder** is the only interface
//! allowed to do that, and it is reached only through the exact role binding of
//! the generation that is supposed to own the value.
//!
//! Each process role holds one direction of each pair:
//!
//! | role | writer handle | commit fragment |
//! |---|---|---|
//! | frontend | encode | decode |
//! | backend | decode | encode |
//!
//! A role therefore structurally cannot forge the carrier it is supposed to
//! consume, nor interpret the one it is supposed to produce.

use std::fmt;
use std::sync::Arc;

use novarocks_proto_models::connector_write as dto;
use novarocks_spi::connector::write_stack::{ConnectorCommitFragment, ConnectorWriterHandle};
use novarocks_spi::connector::{
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorWriteFragmentWireDecoder,
    ConnectorWriteFragmentWireEncoder, ConnectorWriteHandleWireDecoder,
    ConnectorWriteHandleWireEncoder,
};

use super::{ValidatedCommitFragment, ValidatedWriterHandle};
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

/// A codec error keeps the original wire field path and adds the binding owner
/// that selected this codec. It never stores a provider payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteCodecError {
    owner: Arc<str>,
    protocol: ProtocolError,
}

impl ConnectorWriteCodecError {
    pub fn new(owner: impl AsRef<str>, protocol: ProtocolError) -> Self {
        Self {
            owner: Arc::from(owner.as_ref()),
            protocol,
        }
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    pub const fn protocol(&self) -> &ProtocolError {
        &self.protocol
    }

    pub fn invalid(owner: impl AsRef<str>, path: FieldPath, detail: impl Into<String>) -> Self {
        Self::new(
            owner,
            ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail),
        )
    }

    pub fn conflict(owner: impl AsRef<str>, path: FieldPath, detail: impl Into<String>) -> Self {
        Self::new(
            owner,
            ProtocolError::new(path, ProtocolErrorKind::Conflict, detail),
        )
    }
}

impl fmt::Display for ConnectorWriteCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "connector write codec '{}' rejected: {}",
            self.owner, self.protocol
        )
    }
}

impl std::error::Error for ConnectorWriteCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.protocol)
    }
}

impl novarocks_execution_contract::failure::FailureProjection for ConnectorWriteCodecError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        self.protocol.failure_fact()
    }
}

/// Frontend half: turn one logical write recipe into its carrier.
pub trait ConnectorWriteHandleEncoder: Send + Sync {
    fn owner(&self) -> &str;

    fn encode_writer_handle(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<dto::ConnectorWriterHandle, ConnectorWriteCodecError>;

    /// The canonical encoding a budget is charged against.
    ///
    /// It is defined here rather than left to a caller so that the bytes the
    /// frontend charges and the bytes it submits are the same bytes.
    fn canonical_writer_handle_bytes(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<Vec<u8>, ConnectorWriteCodecError> {
        use prost::Message;
        Ok(self.encode_writer_handle(handle)?.encode_to_vec())
    }
}

impl<T> ConnectorWriteHandleEncoder for T
where
    T: ConnectorWriteHandleWireEncoder + ?Sized,
{
    fn owner(&self) -> &str {
        ConnectorWriteHandleWireEncoder::owner(self)
    }

    fn encode_writer_handle(
        &self,
        handle: &ConnectorWriterHandle,
    ) -> Result<dto::ConnectorWriterHandle, ConnectorWriteCodecError> {
        let payload = self
            .encode_writer_handle_payload(handle)
            .map_err(|error| wire_error(self.owner(), error))?;
        Ok(dto::ConnectorWriterHandle {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &payload,
            )),
        })
    }
}

/// Backend half: turn a validated carrier back into a provider recipe.
pub trait ConnectorWriteHandleDecoder: Send + Sync {
    fn owner(&self) -> &str;

    fn decode_writer_handle(
        &self,
        handle: &ValidatedWriterHandle,
    ) -> Result<ConnectorWriterHandle, ConnectorWriteCodecError>;
}

impl<T> ConnectorWriteHandleDecoder for T
where
    T: ConnectorWriteHandleWireDecoder + ?Sized,
{
    fn owner(&self) -> &str {
        ConnectorWriteHandleWireDecoder::owner(self)
    }

    fn decode_writer_handle(
        &self,
        handle: &ValidatedWriterHandle,
    ) -> Result<ConnectorWriterHandle, ConnectorWriteCodecError> {
        self.decode_writer_handle_payload(handle.provider_payload())
            .map_err(|error| wire_error(self.owner(), error))
    }
}

/// Backend half: turn one staged artifact into its carrier.
pub trait ConnectorWriteFragmentEncoder: Send + Sync {
    fn owner(&self) -> &str;

    fn encode_commit_fragment(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<dto::ConnectorCommitFragment, ConnectorWriteCodecError>;

    fn canonical_commit_fragment_bytes(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<Vec<u8>, ConnectorWriteCodecError> {
        use prost::Message;
        Ok(self.encode_commit_fragment(fragment)?.encode_to_vec())
    }
}

impl<T> ConnectorWriteFragmentEncoder for T
where
    T: ConnectorWriteFragmentWireEncoder + ?Sized,
{
    fn owner(&self) -> &str {
        ConnectorWriteFragmentWireEncoder::owner(self)
    }

    fn encode_commit_fragment(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<dto::ConnectorCommitFragment, ConnectorWriteCodecError> {
        let payload = self
            .encode_commit_fragment_payload(fragment)
            .map_err(|error| wire_error(self.owner(), error))?;
        Ok(dto::ConnectorCommitFragment {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &payload,
            )),
        })
    }
}

/// Frontend half: turn a validated carrier back into a provider artifact.
pub trait ConnectorWriteFragmentDecoder: Send + Sync {
    fn owner(&self) -> &str;

    fn decode_commit_fragment(
        &self,
        fragment: &ValidatedCommitFragment,
    ) -> Result<ConnectorCommitFragment, ConnectorWriteCodecError>;
}

impl<T> ConnectorWriteFragmentDecoder for T
where
    T: ConnectorWriteFragmentWireDecoder + ?Sized,
{
    fn owner(&self) -> &str {
        ConnectorWriteFragmentWireDecoder::owner(self)
    }

    fn decode_commit_fragment(
        &self,
        fragment: &ValidatedCommitFragment,
    ) -> Result<ConnectorCommitFragment, ConnectorWriteCodecError> {
        self.decode_commit_fragment_payload(fragment.provider_payload())
            .map_err(|error| wire_error(self.owner(), error))
    }
}

fn wire_error(owner: &str, error: ConnectorCodecError) -> ConnectorWriteCodecError {
    let kind = match error.kind() {
        ConnectorCodecErrorKind::MissingField => ProtocolErrorKind::MissingField,
        ConnectorCodecErrorKind::InvalidEnum => ProtocolErrorKind::InvalidEnum,
        ConnectorCodecErrorKind::InvalidValue | ConnectorCodecErrorKind::UnknownField => {
            ProtocolErrorKind::InvalidValue
        }
        ConnectorCodecErrorKind::DuplicateField => ProtocolErrorKind::DuplicateField,
        ConnectorCodecErrorKind::InconsistentFields => ProtocolErrorKind::InconsistentFields,
        ConnectorCodecErrorKind::VersionMismatch => ProtocolErrorKind::VersionMismatch,
        ConnectorCodecErrorKind::CompileControl(cause) => {
            ProtocolErrorKind::CompileControl(cause.clone())
        }
        ConnectorCodecErrorKind::Domain(_)
        | ConnectorCodecErrorKind::Capacity
        | ConnectorCodecErrorKind::Unsupported => {
            return ConnectorWriteCodecError::new(
                owner,
                ProtocolError::from_domain(FieldPath::root("provider_payload"), error),
            );
        }
    };
    ConnectorWriteCodecError::new(
        owner,
        ProtocolError::new(
            FieldPath::root("provider_payload"),
            kind,
            format!("{}: {}", error.path(), error.detail()),
        ),
    )
}

#[cfg(test)]
mod compile_control_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn provider_domain_adapter_keeps_exact_resource_and_original_codec_path() {
        use novarocks_execution_contract::failure::{FailureProjection, SharedFailureCause};
        use novarocks_spi::connector::{
            ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorFieldPath,
        };
        let mut ledger =
            ConnectorDecodeLedger::new(ConnectorDecodeLimits::try_new(8, 8, 8, 8, 8).unwrap());
        ledger.charge_raw(7).unwrap();
        let original = ledger.charge_raw(2).unwrap_err();
        let codec = ConnectorCodecError::from_domain(
            ConnectorFieldPath::root("columns").index(17),
            original.clone(),
        );
        let fact = original.failure_fact();
        let error = wire_error("iceberg", codec.clone());
        assert_eq!(error.owner(), "iceberg");
        assert_eq!(error.protocol().path().to_string(), "provider_payload");
        assert!(matches!(
            error.protocol().kind(),
            ProtocolErrorKind::Domain(_)
        ));
        assert_eq!(error.protocol().failure_fact(), fact);
        let shared = std::error::Error::source(error.protocol())
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        let preserved = shared.downcast_ref::<ConnectorCodecError>().unwrap();
        assert_eq!(preserved, &codec);
        assert_eq!(preserved.path().to_string(), "columns[17]");
        let leaf = std::error::Error::source(preserved)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert_eq!(leaf.downcast_ref::<ConnectorCodecError>(), Some(&original));
    }

    #[test]
    fn nominal_capacity_and_unsupported_keep_original_fact_quantities_and_source() {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, SharedFailureCause,
        };
        use novarocks_spi::connector::{
            ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorFieldPath,
        };
        let mut ledger =
            ConnectorDecodeLedger::new(ConnectorDecodeLimits::try_new(8, 8, 8, 8, 8).unwrap());
        ledger.charge_raw(7).unwrap();
        let known = ledger.charge_raw(2).unwrap_err();
        let unknown = ConnectorCodecError::new(
            ConnectorFieldPath::root("payload").field("header"),
            ConnectorCodecErrorKind::Capacity,
            "pure compilation was cancelled",
        );
        let unsupported = ConnectorCodecError::new(
            ConnectorFieldPath::root("payload").field("header"),
            ConnectorCodecErrorKind::Unsupported,
            "dependency permission denied",
        );
        for original in [known, unknown, unsupported] {
            let fact = original.failure_fact();
            let error = wire_error("iceberg", original.clone());
            let protocol = error.protocol();
            assert_eq!(error.owner(), "iceberg");
            assert_eq!(protocol.path().to_string(), "provider_payload");
            assert!(matches!(protocol.kind(), ProtocolErrorKind::Domain(_)));
            assert_eq!(protocol.failure_fact(), fact);
            let shared = std::error::Error::source(protocol)
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
                .unwrap();
            assert_eq!(
                shared.downcast_ref::<ConnectorCodecError>(),
                Some(&original)
            );
            if original.kind() == &ConnectorCodecErrorKind::Unsupported {
                assert_eq!(fact.class(), FailureClass::Unsupported);
                assert_eq!(fact.reason().code(), 166);
                assert!(fact.resource().is_none());
            } else {
                assert_eq!(fact.class(), FailureClass::Resource);
                assert_eq!(fact.reason().code(), 167);
                if let Some(resource) = fact.resource() {
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
                } else {
                    assert_eq!(original.path().to_string(), "payload.header");
                }
            }
        }
    }

    #[test]
    fn provider_control_adapter_preserves_exact_typed_cause() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let error = wire_error("paimon", ConnectorCodecError::from(cause.clone()));
            let outer_source = std::error::Error::source(&error).unwrap();
            assert_eq!(
                outer_source.downcast_ref::<ProtocolError>(),
                Some(error.protocol())
            );
            assert_eq!(
                error.protocol().kind(),
                &ProtocolErrorKind::CompileControl(cause.clone())
            );
            assert_eq!(
                std::error::Error::source(error.protocol())
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
        use novarocks_execution_contract::failure::{FailureProjection, SharedFailureCause};
        let original = ConnectorCodecError::new(
            novarocks_spi::connector::ConnectorFieldPath::root("payload"),
            ConnectorCodecErrorKind::Capacity,
            "pure compilation was cancelled",
        );
        let diagnostic = wire_error("paimon", original.clone());
        assert!(matches!(
            diagnostic.protocol().kind(),
            ProtocolErrorKind::Domain(_)
        ));
        assert_eq!(
            diagnostic.protocol().failure_fact(),
            original.failure_fact()
        );
        assert!(diagnostic.protocol().failure_fact().resource().is_none());
        let source = std::error::Error::source(diagnostic.protocol())
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        assert_eq!(
            source.downcast_ref::<ConnectorCodecError>(),
            Some(&original)
        );
    }
}
